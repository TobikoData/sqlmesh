# type: ignore
import os
import pathlib
from unittest import TestCase, mock
from unittest.result import TestResult

import pytest
from pytest_mock.plugin import MockerFixture

from sqlmesh.core import constants as c
from sqlmesh.core.plan import Plan
from sqlmesh.core.user import User, UserRole
from sqlmesh.integrations.github.cicd import command
from sqlmesh.integrations.github.cicd.config import GithubCICDBotConfig, MergeMethod
from sqlmesh.integrations.github.cicd.controller import (
    GithubCheckConclusion,
    GithubCheckStatus,
)
from sqlmesh.utils.errors import ConflictingPlanError, PlanError, TestError, CICDBotError

pytest_plugins = ["tests.integrations.github.cicd.fixtures"]
pytestmark = [
    pytest.mark.github,
    pytest.mark.slow,
]


def test_run_all_success_with_approvers_approved(
    github_client,
    make_controller,
    make_mock_check_run,
    make_mock_issue_comment,
    make_pull_request_review,
    tmp_path: pathlib.Path,
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
        "tests/fixtures/github/pull_request_synchronized.json",
        github_client,
        bot_config=GithubCICDBotConfig(
            invalidate_environment_after_deploy=False, pr_environment_name="MyOverride"
        ),
    )
    controller._context._run_tests = mocker.MagicMock(
        side_effect=lambda **kwargs: (TestResult(), "")
    )
    controller._context.users = [
        User(username="test", github_username="test_github", roles=[UserRole.REQUIRED_APPROVER])
    ]
    controller._context.invalidate_environment = mocker.MagicMock()

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

    assert "SQLMesh - PR Environment Synced" in controller._check_run_mapping
    pr_checks_runs = controller._check_run_mapping["SQLMesh - PR Environment Synced"].all_kwargs
    assert len(pr_checks_runs) == 3
    assert GithubCheckStatus(pr_checks_runs[0]["status"]).is_queued
    assert GithubCheckStatus(pr_checks_runs[1]["status"]).is_in_progress
    assert GithubCheckStatus(pr_checks_runs[2]["status"]).is_completed
    assert GithubCheckConclusion(pr_checks_runs[2]["conclusion"]).is_success

    assert "SQLMesh - Prod Plan Preview" in controller._check_run_mapping
    prod_plan_preview_checks_runs = controller._check_run_mapping[
        "SQLMesh - Prod Plan Preview"
    ].all_kwargs
    assert len(prod_plan_preview_checks_runs) == 3
    assert GithubCheckStatus(prod_plan_preview_checks_runs[0]["status"]).is_queued
    assert GithubCheckStatus(prod_plan_preview_checks_runs[1]["status"]).is_in_progress
    assert GithubCheckStatus(prod_plan_preview_checks_runs[2]["status"]).is_completed
    assert GithubCheckConclusion(prod_plan_preview_checks_runs[2]["conclusion"]).is_success

    assert "SQLMesh - Prod Environment Synced" in controller._check_run_mapping
    prod_checks_runs = controller._check_run_mapping["SQLMesh - Prod Environment Synced"].all_kwargs
    assert len(prod_checks_runs) == 3
    assert GithubCheckStatus(prod_checks_runs[0]["status"]).is_queued
    assert GithubCheckStatus(prod_checks_runs[1]["status"]).is_in_progress
    assert GithubCheckStatus(prod_checks_runs[2]["status"]).is_completed
    assert GithubCheckConclusion(prod_checks_runs[2]["conclusion"]).is_success

    assert "SQLMesh - Has Required Approval" in controller._check_run_mapping
    approval_checks_runs = controller._check_run_mapping[
        "SQLMesh - Has Required Approval"
    ].all_kwargs
    assert len(approval_checks_runs) == 3
    assert GithubCheckStatus(approval_checks_runs[0]["status"]).is_queued
    assert GithubCheckStatus(approval_checks_runs[1]["status"]).is_in_progress
    assert GithubCheckStatus(approval_checks_runs[2]["status"]).is_completed
    assert GithubCheckConclusion(approval_checks_runs[2]["conclusion"]).is_success

    assert len(controller._context.apply.call_args_list) == 2
    pr_plan = controller._context.apply.call_args_list[0][0]
    assert pr_plan[0].environment.name == "myoverride_2"
    prod_plan = controller._context.apply.call_args_list[1][0]
    assert prod_plan[0].environment.name == c.PROD

    assert not mock_pull_request.merge.called
    assert not controller._context.invalidate_environment.called

    assert len(created_comments) == 1
    assert created_comments[0].body.startswith(
        """:robot: **SQLMesh Bot Info** :robot:
- :eyes: To **review** this PR's changes, use virtual data environment:
  - `myoverride_2`
<details>
  <summary>:ship: Prod Plan Being Applied</summary>


**`prod` environment will be initialized**"""
    )
    with open(github_output_file, "r", encoding="utf-8") as f:
        output = f.read()
        assert (
            output
            == "run_unit_tests=success\nhas_required_approval=success\ncreated_pr_environment=true\npr_environment_name=myoverride_2\npr_environment_synced=success\nprod_plan_preview=success\nprod_environment_synced=success\n"
        )


def test_run_all_success_with_approvers_approved_merge_delete(
    github_client,
    make_controller,
    make_mock_check_run,
    make_mock_issue_comment,
    make_pull_request_review,
    tmp_path: pathlib.Path,
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
        "tests/fixtures/github/pull_request_synchronized.json",
        github_client,
        bot_config=GithubCICDBotConfig(merge_method=MergeMethod.REBASE),
    )
    controller._context._run_tests = mocker.MagicMock(
        side_effect=lambda **kwargs: (TestResult(), "")
    )
    controller._context.users = [
        User(username="test", github_username="test_github", roles=[UserRole.REQUIRED_APPROVER])
    ]
    controller._context.invalidate_environment = mocker.MagicMock()

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

    assert "SQLMesh - Prod Plan Preview" in controller._check_run_mapping
    prod_plan_preview_checks_runs = controller._check_run_mapping[
        "SQLMesh - Prod Plan Preview"
    ].all_kwargs
    assert len(prod_plan_preview_checks_runs) == 3
    assert GithubCheckStatus(prod_plan_preview_checks_runs[0]["status"]).is_queued
    assert GithubCheckStatus(prod_plan_preview_checks_runs[1]["status"]).is_in_progress
    assert GithubCheckStatus(prod_plan_preview_checks_runs[2]["status"]).is_completed
    assert GithubCheckConclusion(prod_plan_preview_checks_runs[2]["conclusion"]).is_success

    assert "SQLMesh - PR Environment Synced" in controller._check_run_mapping
    pr_checks_runs = controller._check_run_mapping["SQLMesh - PR Environment Synced"].all_kwargs
    assert len(pr_checks_runs) == 3
    assert GithubCheckStatus(pr_checks_runs[0]["status"]).is_queued
    assert GithubCheckStatus(pr_checks_runs[1]["status"]).is_in_progress
    assert GithubCheckStatus(pr_checks_runs[2]["status"]).is_completed
    assert GithubCheckConclusion(pr_checks_runs[2]["conclusion"]).is_success

    assert "SQLMesh - Prod Environment Synced" in controller._check_run_mapping
    prod_checks_runs = controller._check_run_mapping["SQLMesh - Prod Environment Synced"].all_kwargs
    assert len(prod_checks_runs) == 3
    assert GithubCheckStatus(prod_checks_runs[0]["status"]).is_queued
    assert GithubCheckStatus(prod_checks_runs[1]["status"]).is_in_progress
    assert GithubCheckStatus(prod_checks_runs[2]["status"]).is_completed
    assert GithubCheckConclusion(prod_checks_runs[2]["conclusion"]).is_success

    assert "SQLMesh - Has Required Approval" in controller._check_run_mapping
    approval_checks_runs = controller._check_run_mapping[
        "SQLMesh - Has Required Approval"
    ].all_kwargs
    assert len(approval_checks_runs) == 3
    assert GithubCheckStatus(approval_checks_runs[0]["status"]).is_queued
    assert GithubCheckStatus(approval_checks_runs[1]["status"]).is_in_progress
    assert GithubCheckStatus(approval_checks_runs[2]["status"]).is_completed
    assert GithubCheckConclusion(approval_checks_runs[2]["conclusion"]).is_success

    assert len(controller._context.apply.call_args_list) == 2
    pr_plan = controller._context.apply.call_args_list[0][0]
    assert pr_plan[0].environment.name == "hello_world_2"
    prod_plan = controller._context.apply.call_args_list[1][0]
    assert prod_plan[0].environment.name == c.PROD

    assert mock_pull_request.merge.called
    assert controller._context.invalidate_environment.called

    assert len(created_comments) == 1
    assert created_comments[0].body.startswith(
        """:robot: **SQLMesh Bot Info** :robot:
- :eyes: To **review** this PR's changes, use virtual data environment:
  - `hello_world_2`
<details>
  <summary>:ship: Prod Plan Being Applied</summary>


**`prod` environment will be initialized**"""
    )
    with open(github_output_file, "r", encoding="utf-8") as f:
        output = f.read()
        assert (
            output
            == "run_unit_tests=success\nhas_required_approval=success\ncreated_pr_environment=true\npr_environment_name=hello_world_2\npr_environment_synced=success\nprod_plan_preview=success\nprod_environment_synced=success\n"
        )


def test_run_all_missing_approval(
    github_client,
    make_controller,
    make_mock_check_run,
    make_mock_issue_comment,
    make_pull_request_review,
    tmp_path: pathlib.Path,
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
        "tests/fixtures/github/pull_request_synchronized.json",
        github_client,
        bot_config=GithubCICDBotConfig(invalidate_environment_after_deploy=False),
    )
    controller._context._run_tests = mocker.MagicMock(
        side_effect=lambda **kwargs: (TestResult(), "")
    )
    controller._context.users = [
        User(username="test", github_username="test_github", roles=[UserRole.REQUIRED_APPROVER])
    ]
    controller._context.invalidate_environment = mocker.MagicMock()

    github_output_file = tmp_path / "github_output.txt"

    with mock.patch.dict(os.environ, {"GITHUB_OUTPUT": str(github_output_file)}):
        command._run_all(controller)

    assert "SQLMesh - Prod Plan Preview" in controller._check_run_mapping
    prod_plan_preview_checks_runs = controller._check_run_mapping[
        "SQLMesh - Prod Plan Preview"
    ].all_kwargs
    assert len(prod_plan_preview_checks_runs) == 3
    assert GithubCheckStatus(prod_plan_preview_checks_runs[0]["status"]).is_queued
    assert GithubCheckStatus(prod_plan_preview_checks_runs[1]["status"]).is_in_progress
    assert GithubCheckStatus(prod_plan_preview_checks_runs[2]["status"]).is_completed
    assert GithubCheckConclusion(prod_plan_preview_checks_runs[2]["conclusion"]).is_success

    assert "SQLMesh - PR Environment Synced" in controller._check_run_mapping
    pr_checks_runs = controller._check_run_mapping["SQLMesh - PR Environment Synced"].all_kwargs
    assert len(pr_checks_runs) == 3
    assert GithubCheckStatus(pr_checks_runs[0]["status"]).is_queued
    assert GithubCheckStatus(pr_checks_runs[1]["status"]).is_in_progress
    assert GithubCheckStatus(pr_checks_runs[2]["status"]).is_completed
    assert GithubCheckConclusion(pr_checks_runs[2]["conclusion"]).is_success

    assert "SQLMesh - Prod Environment Synced" in controller._check_run_mapping
    prod_checks_runs = controller._check_run_mapping["SQLMesh - Prod Environment Synced"].all_kwargs
    assert len(prod_checks_runs) == 2
    assert GithubCheckStatus(prod_checks_runs[0]["status"]).is_queued
    assert GithubCheckStatus(prod_checks_runs[1]["status"]).is_completed
    assert GithubCheckConclusion(prod_checks_runs[1]["conclusion"]).is_skipped

    assert "SQLMesh - Run Unit Tests" in controller._check_run_mapping
    test_checks_runs = controller._check_run_mapping["SQLMesh - Run Unit Tests"].all_kwargs
    assert len(test_checks_runs) == 3
    assert GithubCheckStatus(test_checks_runs[0]["status"]).is_queued
    assert GithubCheckStatus(test_checks_runs[1]["status"]).is_in_progress
    assert GithubCheckStatus(test_checks_runs[2]["status"]).is_completed
    assert GithubCheckConclusion(test_checks_runs[2]["conclusion"]).is_success

    assert "SQLMesh - Has Required Approval" in controller._check_run_mapping
    approval_checks_runs = controller._check_run_mapping[
        "SQLMesh - Has Required Approval"
    ].all_kwargs
    assert len(approval_checks_runs) == 3
    assert GithubCheckStatus(approval_checks_runs[0]["status"]).is_queued
    assert GithubCheckStatus(approval_checks_runs[1]["status"]).is_in_progress
    assert GithubCheckStatus(approval_checks_runs[2]["status"]).is_completed
    assert GithubCheckConclusion(approval_checks_runs[2]["conclusion"]).is_neutral

    assert len(controller._context.apply.call_args_list) == 1
    pr_plan = controller._context.apply.call_args_list[0][0]
    assert pr_plan[0].environment.name == "hello_world_2"

    assert not mock_pull_request.merge.called
    assert not controller._context.invalidate_environment.called

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
            == "run_unit_tests=success\nhas_required_approval=neutral\ncreated_pr_environment=true\npr_environment_name=hello_world_2\npr_environment_synced=success\nprod_plan_preview=success\nprod_environment_synced=skipped\n"
        )


def test_run_all_test_failed(
    github_client,
    make_controller,
    make_mock_check_run,
    make_mock_issue_comment,
    make_pull_request_review,
    tmp_path: pathlib.Path,
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
        "tests/fixtures/github/pull_request_synchronized.json",
        github_client,
        bot_config=GithubCICDBotConfig(merge_method=MergeMethod.MERGE),
    )
    test_result = TestResult()
    test_result.testsRun += 1
    test_result.addFailure(TestCase(), (None, None, None))
    controller._context._run_tests = mocker.MagicMock(
        side_effect=lambda **kwargs: (test_result, "some error")
    )
    controller._context.users = [
        User(username="test", github_username="test_github", roles=[UserRole.REQUIRED_APPROVER])
    ]
    controller._context.invalidate_environment = mocker.MagicMock()

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
    assert GithubCheckConclusion(test_checks_runs[2]["conclusion"]).is_failure
    assert test_checks_runs[2]["output"]["title"] == "Tests Failed"
    assert (
        test_checks_runs[2]["output"]["summary"]
        == """**Num Successful Tests: 0**


```some error```


"""
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
        == "Linter or Unit Test(s) failed so skipping creating prod plan"
    )

    assert "SQLMesh - PR Environment Synced" in controller._check_run_mapping
    pr_checks_runs = controller._check_run_mapping["SQLMesh - PR Environment Synced"].all_kwargs
    assert len(pr_checks_runs) == 2
    assert GithubCheckStatus(pr_checks_runs[0]["status"]).is_queued
    assert GithubCheckStatus(pr_checks_runs[1]["status"]).is_completed
    assert GithubCheckConclusion(pr_checks_runs[1]["conclusion"]).is_skipped
    assert pr_checks_runs[1]["output"]["title"] == "PR Virtual Data Environment: hello_world_2"

    assert "SQLMesh - Prod Environment Synced" in controller._check_run_mapping
    prod_checks_runs = controller._check_run_mapping["SQLMesh - Prod Environment Synced"].all_kwargs
    assert len(prod_checks_runs) == 2
    assert GithubCheckStatus(prod_checks_runs[0]["status"]).is_queued
    assert GithubCheckStatus(prod_checks_runs[1]["status"]).is_completed
    assert GithubCheckConclusion(prod_checks_runs[1]["conclusion"]).is_skipped

    assert "SQLMesh - Has Required Approval" in controller._check_run_mapping
    approval_checks_runs = controller._check_run_mapping[
        "SQLMesh - Has Required Approval"
    ].all_kwargs
    assert len(approval_checks_runs) == 3
    assert GithubCheckStatus(approval_checks_runs[0]["status"]).is_queued
    assert GithubCheckStatus(approval_checks_runs[1]["status"]).is_in_progress
    assert GithubCheckStatus(approval_checks_runs[2]["status"]).is_completed
    assert GithubCheckConclusion(approval_checks_runs[2]["conclusion"]).is_success

    assert len(controller._context.apply.call_args_list) == 0

    assert not mock_pull_request.merge.called
    assert not controller._context.invalidate_environment.called

    assert len(created_comments) == 0

    with open(github_output_file, "r", encoding="utf-8") as f:
        output = f.read()
        assert (
            output
            == "run_unit_tests=failure\nhas_required_approval=success\npr_environment_name=hello_world_2\npr_environment_synced=skipped\nprod_plan_preview=skipped\nprod_environment_synced=skipped\n"
        )


def test_run_all_test_exception(
    github_client,
    make_controller,
    make_mock_check_run,
    make_mock_issue_comment,
    make_pull_request_review,
    tmp_path: pathlib.Path,
    mocker: MockerFixture,
):
    """
    Scenario:
    - PR is not merged
    - PR has been approved by a required reviewer
    - Test had an exception (didn't fail but rather had an exception while trying to run)
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
        "tests/fixtures/github/pull_request_synchronized.json",
        github_client,
        bot_config=GithubCICDBotConfig(merge_method=MergeMethod.MERGE),
    )
    test_result = TestResult()
    test_result.addFailure(TestCase(), (None, None, None))
    controller._context._run_tests = mocker.MagicMock(side_effect=TestError("some error"))
    controller._context.users = [
        User(username="test", github_username="test_github", roles=[UserRole.REQUIRED_APPROVER])
    ]
    controller._context.invalidate_environment = mocker.MagicMock()

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
    assert GithubCheckConclusion(test_checks_runs[2]["conclusion"]).is_failure
    assert test_checks_runs[2]["output"]["title"] == "Tests Failed"
    assert (
        test_checks_runs[2]["output"]["summary"]
        .strip()
        .endswith("sqlmesh.utils.errors.TestError: some error")
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
        == "Linter or Unit Test(s) failed so skipping creating prod plan"
    )

    assert "SQLMesh - PR Environment Synced" in controller._check_run_mapping
    pr_checks_runs = controller._check_run_mapping["SQLMesh - PR Environment Synced"].all_kwargs
    assert len(pr_checks_runs) == 2
    assert GithubCheckStatus(pr_checks_runs[0]["status"]).is_queued
    assert GithubCheckStatus(pr_checks_runs[1]["status"]).is_completed
    assert GithubCheckConclusion(pr_checks_runs[1]["conclusion"]).is_skipped
    assert pr_checks_runs[1]["output"]["title"] == "PR Virtual Data Environment: hello_world_2"

    assert "SQLMesh - Prod Environment Synced" in controller._check_run_mapping
    prod_checks_runs = controller._check_run_mapping["SQLMesh - Prod Environment Synced"].all_kwargs
    assert len(prod_checks_runs) == 2
    assert GithubCheckStatus(prod_checks_runs[0]["status"]).is_queued
    assert GithubCheckStatus(prod_checks_runs[1]["status"]).is_completed
    assert GithubCheckConclusion(prod_checks_runs[1]["conclusion"]).is_skipped

    assert "SQLMesh - Has Required Approval" in controller._check_run_mapping
    approval_checks_runs = controller._check_run_mapping[
        "SQLMesh - Has Required Approval"
    ].all_kwargs
    assert len(approval_checks_runs) == 3
    assert GithubCheckStatus(approval_checks_runs[0]["status"]).is_queued
    assert GithubCheckStatus(approval_checks_runs[1]["status"]).is_in_progress
    assert GithubCheckStatus(approval_checks_runs[2]["status"]).is_completed
    assert GithubCheckConclusion(approval_checks_runs[2]["conclusion"]).is_success

    assert len(controller._context.apply.call_args_list) == 0

    assert not mock_pull_request.merge.called
    assert not controller._context.invalidate_environment.called

    assert len(created_comments) == 0

    with open(github_output_file, "r", encoding="utf-8") as f:
        output = f.read()
        assert (
            output
            == "run_unit_tests=failure\nhas_required_approval=success\npr_environment_name=hello_world_2\npr_environment_synced=skipped\nprod_plan_preview=skipped\nprod_environment_synced=skipped\n"
        )


def test_pr_update_failure(
    github_client,
    make_controller,
    make_mock_check_run,
    make_mock_issue_comment,
    make_pull_request_review,
    tmp_path: pathlib.Path,
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
        "tests/fixtures/github/pull_request_synchronized.json",
        github_client,
        bot_config=GithubCICDBotConfig(merge_method=MergeMethod.REBASE),
    )
    controller._context._run_tests = mocker.MagicMock(
        side_effect=lambda **kwargs: (TestResult(), "")
    )
    controller._context.users = [
        User(username="test", github_username="test_github", roles=[UserRole.REQUIRED_APPROVER])
    ]
    controller._context.invalidate_environment = mocker.MagicMock()

    def raise_on_pr_plan(plan: Plan):
        if plan.environment.name == "hello_world_2":
            raise PlanError("Failed to update PR environment")

    controller._context.apply = mocker.MagicMock(side_effect=lambda plan: raise_on_pr_plan(plan))

    github_output_file = tmp_path / "github_output.txt"

    with mock.patch.dict(os.environ, {"GITHUB_OUTPUT": str(github_output_file)}):
        with pytest.raises(CICDBotError):
            command._run_all(controller)

    assert "SQLMesh - PR Environment Synced" in controller._check_run_mapping
    pr_checks_runs = controller._check_run_mapping["SQLMesh - PR Environment Synced"].all_kwargs
    assert len(pr_checks_runs) == 3
    assert GithubCheckStatus(pr_checks_runs[0]["status"]).is_queued
    assert GithubCheckStatus(pr_checks_runs[1]["status"]).is_in_progress
    assert GithubCheckStatus(pr_checks_runs[2]["status"]).is_completed
    assert GithubCheckConclusion(pr_checks_runs[2]["conclusion"]).is_failure

    assert "SQLMesh - Prod Plan Preview" in controller._check_run_mapping
    prod_plan_preview_checks_runs = controller._check_run_mapping[
        "SQLMesh - Prod Plan Preview"
    ].all_kwargs
    assert len(prod_plan_preview_checks_runs) == 2
    assert GithubCheckStatus(prod_plan_preview_checks_runs[0]["status"]).is_queued
    assert GithubCheckStatus(prod_plan_preview_checks_runs[1]["status"]).is_completed
    assert GithubCheckConclusion(prod_plan_preview_checks_runs[1]["conclusion"]).is_skipped

    assert "SQLMesh - Prod Environment Synced" in controller._check_run_mapping
    prod_checks_runs = controller._check_run_mapping["SQLMesh - Prod Environment Synced"].all_kwargs
    assert len(prod_checks_runs) == 2
    assert GithubCheckStatus(prod_checks_runs[0]["status"]).is_queued
    assert GithubCheckStatus(prod_checks_runs[1]["status"]).is_completed
    assert GithubCheckConclusion(prod_checks_runs[1]["conclusion"]).is_skipped

    assert "SQLMesh - Run Unit Tests" in controller._check_run_mapping
    test_checks_runs = controller._check_run_mapping["SQLMesh - Run Unit Tests"].all_kwargs
    assert len(test_checks_runs) == 3
    assert GithubCheckStatus(test_checks_runs[0]["status"]).is_queued
    assert GithubCheckStatus(test_checks_runs[1]["status"]).is_in_progress
    assert GithubCheckStatus(test_checks_runs[2]["status"]).is_completed
    assert GithubCheckConclusion(test_checks_runs[2]["conclusion"]).is_success

    assert "SQLMesh - Has Required Approval" in controller._check_run_mapping
    approval_checks_runs = controller._check_run_mapping[
        "SQLMesh - Has Required Approval"
    ].all_kwargs
    assert len(approval_checks_runs) == 3
    assert GithubCheckStatus(approval_checks_runs[0]["status"]).is_queued
    assert GithubCheckStatus(approval_checks_runs[1]["status"]).is_in_progress
    assert GithubCheckStatus(approval_checks_runs[2]["status"]).is_completed
    assert GithubCheckConclusion(approval_checks_runs[2]["conclusion"]).is_success

    assert len(controller._context.apply.call_args_list) == 1
    pr_plan = controller._context.apply.call_args_list[0][0]
    assert pr_plan[0].environment.name == "hello_world_2"

    assert not mock_pull_request.merge.called
    assert not controller._context.invalidate_environment.called

    assert len(created_comments) == 0

    with open(github_output_file, "r", encoding="utf-8") as f:
        output = f.read()
        assert (
            output
            == "run_unit_tests=success\nhas_required_approval=success\npr_environment_name=hello_world_2\npr_environment_synced=failure\nprod_plan_preview=skipped\nprod_environment_synced=skipped\n"
        )


def make_test_prod_update_failure_case(
    github_client,
    make_controller,
    make_mock_check_run,
    make_mock_issue_comment,
    make_pull_request_review,
    tmp_path: pathlib.Path,
    mocker: MockerFixture,
    to_raise_on_prod_plan: Exception,
    expect_prod_sync_conclusion: GithubCheckConclusion,
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
        "tests/fixtures/github/pull_request_synchronized.json",
        github_client,
        bot_config=GithubCICDBotConfig(merge_method=MergeMethod.REBASE),
    )
    controller._context._run_tests = mocker.MagicMock(
        side_effect=lambda **kwargs: (TestResult(), "")
    )
    controller._context.users = [
        User(username="test", github_username="test_github", roles=[UserRole.REQUIRED_APPROVER])
    ]
    controller._context.invalidate_environment = mocker.MagicMock()

    def raise_on_prod_plan(plan: Plan):
        if plan.environment.name == c.PROD:
            raise to_raise_on_prod_plan

    controller._context.apply = mocker.MagicMock(side_effect=lambda plan: raise_on_prod_plan(plan))

    github_output_file = tmp_path / "github_output.txt"

    with mock.patch.dict(os.environ, {"GITHUB_OUTPUT": str(github_output_file)}):
        with pytest.raises(CICDBotError):
            command._run_all(controller)

    assert "SQLMesh - Prod Plan Preview" in controller._check_run_mapping
    prod_plan_preview_checks_runs = controller._check_run_mapping[
        "SQLMesh - Prod Plan Preview"
    ].all_kwargs
    assert len(prod_plan_preview_checks_runs) == 3
    assert GithubCheckStatus(prod_plan_preview_checks_runs[0]["status"]).is_queued
    assert GithubCheckStatus(prod_plan_preview_checks_runs[1]["status"]).is_in_progress
    assert GithubCheckStatus(prod_plan_preview_checks_runs[2]["status"]).is_completed
    assert GithubCheckConclusion(prod_plan_preview_checks_runs[2]["conclusion"]).is_success

    assert "SQLMesh - PR Environment Synced" in controller._check_run_mapping
    pr_checks_runs = controller._check_run_mapping["SQLMesh - PR Environment Synced"].all_kwargs
    assert len(pr_checks_runs) == 3
    assert GithubCheckStatus(pr_checks_runs[0]["status"]).is_queued
    assert GithubCheckStatus(pr_checks_runs[1]["status"]).is_in_progress
    assert GithubCheckStatus(pr_checks_runs[2]["status"]).is_completed
    assert GithubCheckConclusion(pr_checks_runs[2]["conclusion"]).is_success

    assert "SQLMesh - Prod Environment Synced" in controller._check_run_mapping
    prod_checks_runs = controller._check_run_mapping["SQLMesh - Prod Environment Synced"].all_kwargs
    assert len(prod_checks_runs) == 3
    assert GithubCheckStatus(prod_checks_runs[0]["status"]).is_queued
    assert GithubCheckStatus(prod_checks_runs[1]["status"]).is_in_progress
    assert GithubCheckStatus(prod_checks_runs[2]["status"]).is_completed
    assert GithubCheckConclusion(prod_checks_runs[2]["conclusion"]) == expect_prod_sync_conclusion

    assert "SQLMesh - Run Unit Tests" in controller._check_run_mapping
    test_checks_runs = controller._check_run_mapping["SQLMesh - Run Unit Tests"].all_kwargs
    assert len(test_checks_runs) == 3
    assert GithubCheckStatus(test_checks_runs[0]["status"]).is_queued
    assert GithubCheckStatus(test_checks_runs[1]["status"]).is_in_progress
    assert GithubCheckStatus(test_checks_runs[2]["status"]).is_completed
    assert GithubCheckConclusion(test_checks_runs[2]["conclusion"]).is_success

    assert "SQLMesh - Has Required Approval" in controller._check_run_mapping
    approval_checks_runs = controller._check_run_mapping[
        "SQLMesh - Has Required Approval"
    ].all_kwargs
    assert len(approval_checks_runs) == 3
    assert GithubCheckStatus(approval_checks_runs[0]["status"]).is_queued
    assert GithubCheckStatus(approval_checks_runs[1]["status"]).is_in_progress
    assert GithubCheckStatus(approval_checks_runs[2]["status"]).is_completed
    assert GithubCheckConclusion(approval_checks_runs[2]["conclusion"]).is_success

    assert len(controller._context.apply.call_args_list) == 2
    pr_plan = controller._context.apply.call_args_list[0][0]
    assert pr_plan[0].environment.name == "hello_world_2"
    prod_plan = controller._context.apply.call_args_list[1][0]
    assert prod_plan[0].environment.name == c.PROD

    assert not mock_pull_request.merge.called
    assert not controller._context.invalidate_environment.called

    assert len(created_comments) == 1
    assert created_comments[0].body.startswith(
        """:robot: **SQLMesh Bot Info** :robot:
- :eyes: To **review** this PR's changes, use virtual data environment:
  - `hello_world_2`
<details>
  <summary>:ship: Prod Plan Being Applied</summary>


**`prod` environment will be initialized**"""
    )

    with open(github_output_file, "r", encoding="utf-8") as f:
        output = f.read()
        assert (
            output
            == f"run_unit_tests=success\nhas_required_approval=success\ncreated_pr_environment=true\npr_environment_name=hello_world_2\npr_environment_synced=success\nprod_plan_preview=success\nprod_environment_synced={expect_prod_sync_conclusion.value}\n"
        )


def test_prod_update_failure(
    github_client,
    make_controller,
    make_mock_check_run,
    make_mock_issue_comment,
    make_pull_request_review,
    tmp_path: pathlib.Path,
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

    make_test_prod_update_failure_case(
        github_client=github_client,
        make_controller=make_controller,
        make_mock_check_run=make_mock_check_run,
        make_mock_issue_comment=make_mock_issue_comment,
        make_pull_request_review=make_pull_request_review,
        tmp_path=tmp_path,
        mocker=mocker,
        to_raise_on_prod_plan=PlanError("Failed to update Prod environment"),
        expect_prod_sync_conclusion=GithubCheckConclusion.ACTION_REQUIRED,
    )


def test_prod_update_conflict(
    github_client,
    make_controller,
    make_mock_check_run,
    make_mock_issue_comment,
    make_pull_request_review,
    tmp_path: pathlib.Path,
    mocker: MockerFixture,
):
    """
    Scenario:
    - PR is not merged
    - PR has been approved by a required reviewer
    - Tests passed
    - PR Merge Method defined
    - Delete environment is enabled
    - Prod environment update conflicted
    """

    make_test_prod_update_failure_case(
        github_client=github_client,
        make_controller=make_controller,
        make_mock_check_run=make_mock_check_run,
        make_mock_issue_comment=make_mock_issue_comment,
        make_pull_request_review=make_pull_request_review,
        tmp_path=tmp_path,
        mocker=mocker,
        to_raise_on_prod_plan=ConflictingPlanError("Plan a conflicts with plan b"),
        expect_prod_sync_conclusion=GithubCheckConclusion.SKIPPED,
    )


def test_prod_update_exception(
    github_client,
    make_controller,
    make_mock_check_run,
    make_mock_issue_comment,
    make_pull_request_review,
    tmp_path: pathlib.Path,
    mocker: MockerFixture,
):
    """
    Scenario:
    - PR is not merged
    - PR has been approved by a required reviewer
    - Tests passed
    - PR Merge Method defined
    - Delete environment is enabled
    - Prod environment update fails with an unknown exception
    """

    make_test_prod_update_failure_case(
        github_client=github_client,
        make_controller=make_controller,
        make_mock_check_run=make_mock_check_run,
        make_mock_issue_comment=make_mock_issue_comment,
        make_pull_request_review=make_pull_request_review,
        tmp_path=tmp_path,
        mocker=mocker,
        to_raise_on_prod_plan=RuntimeError("boom"),
        expect_prod_sync_conclusion=GithubCheckConclusion.FAILURE,
    )


def test_comment_command_invalid(
    github_client,
    make_controller,
    make_mock_check_run,
    make_mock_issue_comment,
    make_pull_request_review,
    make_event_issue_comment,
    tmp_path: pathlib.Path,
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
        make_event_issue_comment("created", "not a command"),
        github_client,
        bot_config=GithubCICDBotConfig(merge_method=MergeMethod.REBASE),
    )
    controller._context._run_tests = mocker.MagicMock(
        side_effect=lambda **kwargs: (TestResult(), "")
    )
    controller._context.users = [
        User(username="test", github_username="test_github", roles=[UserRole.REQUIRED_APPROVER])
    ]
    controller._context.invalidate_environment = mocker.MagicMock()

    github_output_file = tmp_path / "github_output.txt"

    github_output_file.touch()

    with mock.patch.dict(os.environ, {"GITHUB_OUTPUT": str(github_output_file)}):
        command._run_all(controller)

    assert len(controller._check_run_mapping) == 0

    assert not mock_pull_request.merge.called
    assert not controller._context.invalidate_environment.called

    assert len(created_comments) == 0

    with open(github_output_file, "r", encoding="utf-8") as f:
        output = f.read()
        assert output == ""


def test_comment_command_deploy_prod(
    github_client,
    make_controller,
    make_mock_check_run,
    make_mock_issue_comment,
    make_event_issue_comment,
    tmp_path: pathlib.Path,
    mocker: MockerFixture,
):
    """
    Scenario:
    - PR is not merged
    - Required approvers defined but approval not given
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

    controller = make_controller(
        make_event_issue_comment("created", "/deploy"),
        github_client,
        bot_config=GithubCICDBotConfig(merge_method=MergeMethod.REBASE, enable_deploy_command=True),
    )
    controller._context._run_tests = mocker.MagicMock(
        side_effect=lambda **kwargs: (TestResult(), "")
    )
    controller._context.users = [
        User(username="test", github_username="test_github", roles=[UserRole.REQUIRED_APPROVER])
    ]
    controller._context.invalidate_environment = mocker.MagicMock()

    github_output_file = tmp_path / "github_output.txt"

    with mock.patch.dict(os.environ, {"GITHUB_OUTPUT": str(github_output_file)}):
        command._run_all(controller)

    assert "SQLMesh - Prod Plan Preview" in controller._check_run_mapping
    prod_plan_preview_checks_runs = controller._check_run_mapping[
        "SQLMesh - Prod Plan Preview"
    ].all_kwargs
    assert len(prod_plan_preview_checks_runs) == 3
    assert GithubCheckStatus(prod_plan_preview_checks_runs[0]["status"]).is_queued
    assert GithubCheckStatus(prod_plan_preview_checks_runs[1]["status"]).is_in_progress
    assert GithubCheckStatus(prod_plan_preview_checks_runs[2]["status"]).is_completed
    assert GithubCheckConclusion(prod_plan_preview_checks_runs[2]["conclusion"]).is_success

    assert "SQLMesh - PR Environment Synced" in controller._check_run_mapping
    pr_checks_runs = controller._check_run_mapping["SQLMesh - PR Environment Synced"].all_kwargs
    assert len(pr_checks_runs) == 3
    assert GithubCheckStatus(pr_checks_runs[0]["status"]).is_queued
    assert GithubCheckStatus(pr_checks_runs[1]["status"]).is_in_progress
    assert GithubCheckStatus(pr_checks_runs[2]["status"]).is_completed
    assert GithubCheckConclusion(pr_checks_runs[2]["conclusion"]).is_success

    assert "SQLMesh - Prod Environment Synced" in controller._check_run_mapping
    prod_checks_runs = controller._check_run_mapping["SQLMesh - Prod Environment Synced"].all_kwargs
    assert len(prod_checks_runs) == 3
    assert GithubCheckStatus(prod_checks_runs[0]["status"]).is_queued
    assert GithubCheckStatus(prod_checks_runs[1]["status"]).is_in_progress
    assert GithubCheckStatus(prod_checks_runs[2]["status"]).is_completed
    assert GithubCheckConclusion(prod_checks_runs[2]["conclusion"]).is_success

    assert "SQLMesh - Run Unit Tests" in controller._check_run_mapping
    test_checks_runs = controller._check_run_mapping["SQLMesh - Run Unit Tests"].all_kwargs
    assert len(test_checks_runs) == 3
    assert GithubCheckStatus(test_checks_runs[0]["status"]).is_queued
    assert GithubCheckStatus(test_checks_runs[1]["status"]).is_in_progress
    assert GithubCheckStatus(test_checks_runs[2]["status"]).is_completed
    assert GithubCheckConclusion(test_checks_runs[2]["conclusion"]).is_success

    # Required Approvers are defined and approval is not given but we use the deploy command as a sign
    # of approval and therefore this stage is skipped
    # Improvement: Have this display skipped and the reason why
    assert "SQLMesh - Has Required Approval" not in controller._check_run_mapping

    assert len(controller._context.apply.call_args_list) == 2
    pr_plan = controller._context.apply.call_args_list[0][0]
    assert pr_plan[0].environment.name == "hello_world_2"
    prod_plan = controller._context.apply.call_args_list[1][0]
    assert prod_plan[0].environment.name == c.PROD

    assert mock_pull_request.merge.called
    assert controller._context.invalidate_environment.called

    assert len(created_comments) == 1
    assert created_comments[0].body.startswith(
        """:robot: **SQLMesh Bot Info** :robot:
- :eyes: To **review** this PR's changes, use virtual data environment:
  - `hello_world_2`
- :arrow_forward: To **apply** this PR's plan to prod, comment:
  - `/deploy`
<details>
  <summary>:ship: Prod Plan Being Applied</summary>


**`prod` environment will be initialized**"""
    )

    with open(github_output_file, "r", encoding="utf-8") as f:
        output = f.read()
        assert (
            output
            == "run_unit_tests=success\ncreated_pr_environment=true\npr_environment_name=hello_world_2\npr_environment_synced=success\nprod_plan_preview=success\nprod_environment_synced=success\n"
        )


def test_comment_command_deploy_prod_not_enabled(
    github_client,
    make_controller,
    make_mock_check_run,
    make_mock_issue_comment,
    make_event_issue_comment,
    tmp_path: pathlib.Path,
    mocker: MockerFixture,
):
    """
    Scenario:
    - PR is not merged
    - No required approvers defined
    - Tests passed
    - PR Merge Method defined
    - Delete environment is enabled
    - Comment triggered this action and it is a valid deploy comment but deploy command is not enabled
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

    controller = make_controller(
        make_event_issue_comment("created", "/deploy"),
        github_client,
        bot_config=GithubCICDBotConfig(merge_method=MergeMethod.REBASE),
    )
    controller._context._run_tests = mocker.MagicMock(
        side_effect=lambda **kwargs: (TestResult(), "")
    )
    controller._context.users = []
    controller._context.invalidate_environment = mocker.MagicMock()

    github_output_file = tmp_path / "github_output.txt"

    github_output_file.touch()

    with mock.patch.dict(os.environ, {"GITHUB_OUTPUT": str(github_output_file)}):
        command._run_all(controller)

    assert controller._check_run_mapping == {}

    assert len(controller._context.apply.call_args_list) == 0

    assert not mock_pull_request.merge.called
    assert not controller._context.invalidate_environment.called

    assert len(created_comments) == 0

    with open(github_output_file, "r", encoding="utf-8") as f:
        output = f.read()
        assert output == ""
